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

package org.apache.iggy.system;

import java.util.Optional;

/** Whether a metadata changes answer pages change records or current state items. */
public enum MetadataChangePhase {
    /** Change records, the answer to earliest, latest and after. */
    CHANGES(0, "changes"),
    /** Current state items, the answer to current. */
    CURRENT(1, "current");

    private final int code;
    private final String value;

    MetadataChangePhase(int code, String value) {
        this.code = code;
        this.value = value;
    }

    /** The phase with wire code {@code code}, or empty for a phase this build does not know. */
    public static Optional<MetadataChangePhase> fromCode(int code) {
        for (MetadataChangePhase phase : values()) {
            if (phase.code == code) {
                return Optional.of(phase);
            }
        }
        return Optional.empty();
    }

    /** The phase named {@code value}, or empty for a phase this build does not know. */
    public static Optional<MetadataChangePhase> fromValue(String value) {
        for (MetadataChangePhase phase : values()) {
            if (phase.value.equals(value)) {
                return Optional.of(phase);
            }
        }
        return Optional.empty();
    }

    public int asCode() {
        return code;
    }

    public String value() {
        return value;
    }
}
