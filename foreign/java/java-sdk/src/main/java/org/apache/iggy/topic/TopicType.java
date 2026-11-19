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

/** What a topic is, fixed at create and never changed afterwards. */
public enum TopicType {
    /** The topic's messages stay in this cluster. */
    NORMAL("normal"),
    /** The topic's messages also become readable in the other datacenters, through the object store. */
    GEO("geo");

    private final String value;

    TopicType(String value) {
        this.value = value;
    }

    /** The topic type named {@code value}, or empty for a topic type this build does not know. */
    public static Optional<TopicType> fromValue(String value) {
        for (TopicType type : values()) {
            if (type.value.equals(value)) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }

    public String value() {
        return value;
    }
}
