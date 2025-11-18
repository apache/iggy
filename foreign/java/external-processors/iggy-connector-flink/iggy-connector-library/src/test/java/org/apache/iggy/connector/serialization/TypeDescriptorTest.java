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

import org.junit.jupiter.api.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TypeDescriptorTest {

    @Test
    void shouldCreateDescriptorWithSimpleClass() {
        TypeDescriptor<String> descriptor = TypeDescriptor.of(String.class);

        assertThat(descriptor.getTypeClass()).isEqualTo(String.class);
        assertThat(descriptor.getGenericType()).isEqualTo(String.class);
    }

    @Test
    void shouldCreateDescriptorWithGenericType() {
        Type genericType = new ParameterizedType() {
            @Override
            public Type[] getActualTypeArguments() {
                return new Type[] {String.class};
            }

            @Override
            public Type getRawType() {
                return List.class;
            }

            @Override
            public Type getOwnerType() {
                return null;
            }
        };

        @SuppressWarnings("unchecked")
        TypeDescriptor<List> descriptor = TypeDescriptor.of((Class<List>) List.class, genericType);

        assertThat(descriptor.getTypeClass()).isEqualTo(List.class);
        assertThat(descriptor.getGenericType()).isEqualTo(genericType);
    }

    @Test
    void shouldUseTypeClassAsGenericTypeWhenGenericTypeIsNull() {
        TypeDescriptor<String> descriptor = TypeDescriptor.of(String.class, null);

        assertThat(descriptor.getTypeClass()).isEqualTo(String.class);
        assertThat(descriptor.getGenericType()).isEqualTo(String.class);
    }

    @Test
    void shouldThrowExceptionWhenTypeClassIsNull() {
        assertThatThrownBy(() -> TypeDescriptor.of(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("typeClass must not be null");
    }

    @Test
    void shouldSupportPrimitiveTypes() {
        TypeDescriptor<Integer> intDescriptor = TypeDescriptor.of(Integer.class);
        TypeDescriptor<Long> longDescriptor = TypeDescriptor.of(Long.class);
        TypeDescriptor<Double> doubleDescriptor = TypeDescriptor.of(Double.class);
        TypeDescriptor<Boolean> booleanDescriptor = TypeDescriptor.of(Boolean.class);

        assertThat(intDescriptor.getTypeClass()).isEqualTo(Integer.class);
        assertThat(longDescriptor.getTypeClass()).isEqualTo(Long.class);
        assertThat(doubleDescriptor.getTypeClass()).isEqualTo(Double.class);
        assertThat(booleanDescriptor.getTypeClass()).isEqualTo(Boolean.class);
    }

    @Test
    void shouldSupportCustomClasses() {
        TypeDescriptor<CustomClass> descriptor = TypeDescriptor.of(CustomClass.class);

        assertThat(descriptor.getTypeClass()).isEqualTo(CustomClass.class);
    }

    @Test
    void shouldImplementEqualsCorrectly() {
        TypeDescriptor<String> descriptor1 = TypeDescriptor.of(String.class);
        TypeDescriptor<String> descriptor2 = TypeDescriptor.of(String.class);
        TypeDescriptor<Integer> descriptor3 = TypeDescriptor.of(Integer.class);

        assertThat(descriptor1).isEqualTo(descriptor2);
        assertThat(descriptor1).hasSameHashCodeAs(descriptor2);
        assertThat(descriptor1).isNotEqualTo(descriptor3);
        assertThat(descriptor1).isEqualTo(descriptor1);
        assertThat(descriptor1).isNotEqualTo(null);
        assertThat(descriptor1).isNotEqualTo(new Object());
    }

    @Test
    void shouldConsiderGenericTypeInEquals() {
        Type genericType1 = new ParameterizedType() {
            @Override
            public Type[] getActualTypeArguments() {
                return new Type[] {String.class};
            }

            @Override
            public Type getRawType() {
                return List.class;
            }

            @Override
            public Type getOwnerType() {
                return null;
            }
        };

        Type genericType2 = new ParameterizedType() {
            @Override
            public Type[] getActualTypeArguments() {
                return new Type[] {Integer.class};
            }

            @Override
            public Type getRawType() {
                return List.class;
            }

            @Override
            public Type getOwnerType() {
                return null;
            }
        };

        @SuppressWarnings("unchecked")
        TypeDescriptor<List> descriptor1 = TypeDescriptor.of((Class<List>) List.class, genericType1);
        @SuppressWarnings("unchecked")
        TypeDescriptor<List> descriptor2 = TypeDescriptor.of((Class<List>) List.class, genericType2);

        assertThat(descriptor1).isNotEqualTo(descriptor2);
    }

    @Test
    void shouldImplementToStringCorrectly() {
        TypeDescriptor<String> descriptor = TypeDescriptor.of(String.class);

        String toString = descriptor.toString();
        assertThat(toString).contains("TypeDescriptor");
        assertThat(toString).contains("String");
    }

    @Test
    void shouldBeSerializable() {
        TypeDescriptor<String> descriptor = TypeDescriptor.of(String.class);
        assertThat(descriptor).isInstanceOf(java.io.Serializable.class);
    }

    // Helper class for testing custom types
    private static final class CustomClass {
        private String value;
    }
}
