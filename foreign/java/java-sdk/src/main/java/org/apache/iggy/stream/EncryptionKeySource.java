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

import java.util.Optional;

/** Who chose a stream's data encryption key, as the server reports it. */
public enum EncryptionKeySource {
    /** The client sent the key when it created the stream. */
    STREAM("stream"),
    /** The server generated the key because encryption was enabled and the client sent none. */
    SERVER("server"),
    /** The stream is stored in plaintext. */
    NONE("none");

    private final String value;

    EncryptionKeySource(String value) {
        this.value = value;
    }

    /** The source named {@code value}, or empty for a source this build does not know. */
    public static Optional<EncryptionKeySource> fromValue(String value) {
        for (EncryptionKeySource source : values()) {
            if (source.value.equals(value)) {
                return Optional.of(source);
            }
        }
        return Optional.empty();
    }

    public String value() {
        return value;
    }
}
