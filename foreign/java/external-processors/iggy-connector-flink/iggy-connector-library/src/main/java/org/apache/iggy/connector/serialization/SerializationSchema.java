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
import java.util.Optional;

/**
 * Framework-agnostic interface for serializing objects to Iggy messages.
 * This interface is designed to be wrapped by framework-specific
 * serialization adapters.
 *
 * @param <T> The type of the object to serialize
 */
public interface SerializationSchema<T> extends Serializable {

    /**
     * Serializes an object to byte array for sending to Iggy.
     *
     * @param element The object to serialize
     * @return The serialized message as byte array
     * @throws IOException If serialization fails
     */
    byte[] serialize(T element) throws IOException;

    /**
     * Extracts a partition key from the element.
     * If present, this key will be used to determine which partition
     * the message should be sent to.
     *
     * @param element The object to extract key from
     * @return Optional partition key
     */
    default Optional<Integer> extractPartitionKey(T element) {
        return Optional.empty();
    }

    /**
     * Returns whether this schema can handle null values.
     * If true, the serialize method may be called with null element.
     *
     * @return true if null values are supported, false otherwise
     */
    default boolean isNullable() {
        return false;
    }
}
