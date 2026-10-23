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

import org.apache.iggy.system.OptionKey;

import static org.apache.iggy.system.OptionSpec.FLAG_RESETTABLE;
import static org.apache.iggy.system.OptionSpec.FLAG_UPDATABLE;

/**
 * The option keys of a topic, in catalog order.
 *
 * <p>An update may set {@code name}, {@code message_expiry} and {@code max_topic_size}, and reset the
 * last two to their defaults. Every other key is fixed at create.
 */
public enum TopicOptionKey implements OptionKey {
    NAME("name", FLAG_UPDATABLE),
    COMPRESSION_ALGORITHM("compression_algorithm", 0),
    MESSAGE_EXPIRY("message_expiry", FLAG_UPDATABLE | FLAG_RESETTABLE),
    MAX_TOPIC_SIZE("max_topic_size", FLAG_UPDATABLE | FLAG_RESETTABLE),
    SEGMENT_SIZE("segment_size", 0),
    DURABILITY("durability", 0),
    CONSUMER_OFFSET_DURABILITY("consumer_offset_durability", 0),
    PARTITION_RESIZE_POLICY("partition_resize_policy", 0),
    MESSAGES_REQUIRED_TO_SAVE("messages_required_to_save", 0),
    SIZE_OF_MESSAGES_REQUIRED_TO_SAVE("size_of_messages_required_to_save", 0),
    PREALLOCATE_SEGMENTS("preallocate_segments", 0);

    private final String value;
    private final int flags;

    TopicOptionKey(String value, int flags) {
        this.value = value;
        this.flags = flags;
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public int flags() {
        return flags;
    }
}
