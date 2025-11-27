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

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Deserialization schema for JSON using Jackson.
 * Supports Java 8 time types and handles unknown properties gracefully.
 *
 * @param <T> the type to deserialize to
 */
public class JsonDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> typeClass;
    private final transient ObjectMapper objectMapper;

    /**
     * Creates a new JSON deserializer for the specified type.
     *
     * @param typeClass the class to deserialize to
     */
    public JsonDeserializationSchema(Class<T> typeClass) {
        this(typeClass, new JsonMapper());
    }

    /**
     * Creates a new JSON deserializer with a custom ObjectMapper.
     *
     * @param typeClass    the class to deserialize to
     * @param objectMapper the Jackson ObjectMapper to use
     */
    public JsonDeserializationSchema(Class<T> typeClass, ObjectMapper objectMapper) {
        if (typeClass == null) {
            throw new IllegalArgumentException("typeClass cannot be null");
        }
        if (objectMapper == null) {
            throw new IllegalArgumentException("objectMapper cannot be null");
        }
        this.typeClass = typeClass;
        this.objectMapper = objectMapper;
    }

    @Override
    public T deserialize(byte[] data, RecordMetadata metadata) {
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            return getObjectMapper().readValue(data, typeClass);
        } catch (JacksonException e) {
            throw new RuntimeException("Failed to deserialize JSON for type " + typeClass.getName(), e);
        }
    }

    @Override
    public TypeDescriptor<T> getProducedType() {
        return TypeDescriptor.of(typeClass);
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    /**
     * Gets the ObjectMapper, creating a default one if needed (for deserialization).
     *
     * @return the ObjectMapper instance
     */
    private ObjectMapper getObjectMapper() {
        if (objectMapper != null) {
            return objectMapper;
        }
        // Fallback for deserialized instances
        return new JsonMapper();
    }

    /**
     * Gets the target type class.
     *
     * @return the type class
     */
    public Class<T> getTypeClass() {
        return typeClass;
    }
}
