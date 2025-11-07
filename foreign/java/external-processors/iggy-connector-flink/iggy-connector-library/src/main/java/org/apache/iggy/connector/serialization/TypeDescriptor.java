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

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Objects;

/**
 * Framework-agnostic type descriptor.
 * This can be mapped to framework-specific type systems
 * (e.g., Flink's TypeInformation, Spark's Encoder).
 *
 * @param <T> The described type
 */
public final class TypeDescriptor<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Class<T> typeClass;
    private final Type genericType;

    private TypeDescriptor(Class<T> typeClass, Type genericType) {
        this.typeClass = Objects.requireNonNull(typeClass, "typeClass must not be null");
        this.genericType = genericType != null ? genericType : typeClass;
    }

    /**
     * Creates a type descriptor for a simple class.
     */
    public static <T> TypeDescriptor<T> of(Class<T> typeClass) {
        return new TypeDescriptor<>(typeClass, typeClass);
    }

    /**
     * Creates a type descriptor with generic type information.
     */
    public static <T> TypeDescriptor<T> of(Class<T> typeClass, Type genericType) {
        return new TypeDescriptor<>(typeClass, genericType);
    }

    public Class<T> getTypeClass() {
        return typeClass;
    }

    public Type getGenericType() {
        return genericType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TypeDescriptor<?> that = (TypeDescriptor<?>) o;
        return Objects.equals(typeClass, that.typeClass) && Objects.equals(genericType, that.genericType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeClass, genericType);
    }

    @Override
    public String toString() {
        return "TypeDescriptor{" + "typeClass=" + typeClass + ", genericType=" + genericType + '}';
    }
}
