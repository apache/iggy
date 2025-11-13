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

package org.apache.iggy.connector.serialization;

import java.io.IOException;
import java.io.Serializable;

/**
 * Framework-agnostic interface for deserializing Iggy messages.
 * This interface is designed to be wrapped by framework-specific
 * deserialization adapters (e.g., Flink's DeserializationSchema,
 * Spark's Decoder).
 *
 * @param <T> The type of the deserialized object
 */
public interface DeserializationSchema<T> extends Serializable {

    /**
     * Deserializes a message from byte array to the target type.
     *
     * @param data The message data as byte array
     * @param metadata Metadata about the message (offset, partition, timestamp, etc.)
     * @return The deserialized object
     * @throws IOException If deserialization fails
     */
    T deserialize(byte[] data, RecordMetadata metadata) throws IOException;

    /**
     * Returns type information about the produced type.
     * This is framework-agnostic type information that can be
     * mapped to framework-specific type systems.
     *
     * @return Type descriptor for the produced type
     */
    TypeDescriptor<T> getProducedType();

    /**
     * Returns whether this schema can handle null values.
     * If true, the deserialize method may be called with null data.
     *
     * @return true if null values are supported, false otherwise
     */
    default boolean isNullable() {
        return false;
    }
}
