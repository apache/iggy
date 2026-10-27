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

/** Outcome of a metadata changes query. */
public enum MetadataChangeStatus {
    /** Records or items after the requested position. */
    OK(0, "ok"),
    /** The answering node has not applied the requested position yet. Ask again later or ask another node. */
    BEHIND(1, "behind"),
    /** The node no longer holds every record after the position. Rebuild from current state. */
    EXPIRED(2, "expired"),
    /** The position belongs to another history. Rebuild from current state. */
    RESET(3, "reset");

    private final int code;
    private final String value;

    MetadataChangeStatus(int code, String value) {
        this.code = code;
        this.value = value;
    }

    /** The status with wire code {@code code}, or empty for a status this build does not know. */
    public static Optional<MetadataChangeStatus> fromCode(int code) {
        for (MetadataChangeStatus status : values()) {
            if (status.code == code) {
                return Optional.of(status);
            }
        }
        return Optional.empty();
    }

    /** The status named {@code value}, or empty for a status this build does not know. */
    public static Optional<MetadataChangeStatus> fromValue(String value) {
        for (MetadataChangeStatus status : values()) {
            if (status.value.equals(value)) {
                return Optional.of(status);
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
