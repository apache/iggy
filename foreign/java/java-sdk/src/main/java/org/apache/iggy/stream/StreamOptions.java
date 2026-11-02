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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The options a stream is created with.
 *
 * <p>{@code encryption_key} gives the stream its own 32-byte data key. It must be 32 random bytes. It
 * travels inside the create request over whatever transport the client uses, so protecting it on the
 * network is the job of TLS on that transport. The server stores it wrapped and never returns it, reads
 * report only its source and fingerprints. Without a key, a server with encryption enabled generates one
 * for the stream.
 *
 * <pre>{@code
 * var options = StreamOptions.builder().encryptionKey(key).build();
 * streamsClient.createStream("payments", options);
 * }</pre>
 */
public final class StreamOptions {

    private final byte[] encryptionKey;
    private final Map<String, HeaderValue> raw;

    private StreamOptions(byte[] encryptionKey, Map<String, HeaderValue> raw) {
        this.encryptionKey = encryptionKey;
        this.raw = raw;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** No options: the server encryption setting decides whether the stream gets a generated key. */
    public static StreamOptions empty() {
        return builder().build();
    }

    /** A copy of the stream key, when one is set. */
    public Optional<byte[]> encryptionKey() {
        return Optional.ofNullable(encryptionKey).map(byte[]::clone);
    }

    /** Keys set for options newer than this build. */
    public Map<String, HeaderValue> raw() {
        return raw;
    }

    /**
     * Every option this value sends, keyed by option name. A typed setting wins over a raw entry of the
     * same key.
     */
    public Map<String, HeaderValue> entries() {
        Map<String, HeaderValue> entries = new LinkedHashMap<>(raw);
        if (encryptionKey != null) {
            entries.put(StreamOptionKey.ENCRYPTION_KEY.value(), HeaderValue.fromRaw(encryptionKey.clone()));
        }
        return Collections.unmodifiableMap(entries);
    }

    @Override
    public String toString() {
        return "StreamOptions[encryptionKey=" + (encryptionKey == null ? "none" : "<redacted>") + ", raw="
                + raw.keySet() + "]";
    }

    public static final class Builder {

        private final Map<String, HeaderValue> raw = new LinkedHashMap<>();
        private byte[] encryptionKey;

        private Builder() {}

        /**
         * The stream's own data key, exactly 32 random bytes.
         *
         * @throws org.apache.iggy.exception.IggyValidationException when the key is not 32 bytes
         */
        public Builder encryptionKey(byte[] key) {
            encryptionKey = EncryptionKeys.requireValid(key).clone();
            return this;
        }

        /**
         * The stream's own data key as canonical standard padded base64 of 32 bytes.
         *
         * @throws org.apache.iggy.exception.IggyValidationException when the text is not the
         *     canonical encoding of 32 bytes
         */
        public Builder encryptionKeyBase64(String key) {
            encryptionKey = EncryptionKeys.decodeBase64(key);
            return this;
        }

        /** An option this build has no setter for. A typed setter wins on collision. */
        public Builder raw(String key, HeaderValue value) {
            raw.put(key, value);
            return this;
        }

        public StreamOptions build() {
            return new StreamOptions(encryptionKey, Collections.unmodifiableMap(new LinkedHashMap<>(raw)));
        }
    }
}
