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

import java.util.Optional;

/** How a geo topic writes its messages. Only a geo topic has a mode. */
public enum GeoMode {
    /** A batch goes to the object store before it commits. */
    GEO("geo"),
    /** A batch is replicated first, and sealed segments go to the object store later. */
    GEO_TIERED("geo_tiered");

    private final String value;

    GeoMode(String value) {
        this.value = value;
    }

    /** The mode named {@code value}, or empty for a mode this build does not know. */
    public static Optional<GeoMode> fromValue(String value) {
        for (GeoMode mode : values()) {
            if (mode.value.equals(value)) {
                return Optional.of(mode);
            }
        }
        return Optional.empty();
    }

    public String value() {
        return value;
    }
}
