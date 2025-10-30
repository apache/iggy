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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Deserialization schema for UTF-8 strings.
 */
public class StringDeserializationSchema implements DeserializationSchema<String> {

    private static final long serialVersionUID = 1L;

    private final String charsetName;
    private transient Charset charset;

    /**
     * Creates a new string deserializer using UTF-8 encoding.
     */
    public StringDeserializationSchema() {
        this(StandardCharsets.UTF_8);
    }

    /**
     * Creates a new string deserializer with specified charset.
     *
     * @param charset the character encoding to use
     */
    public StringDeserializationSchema(Charset charset) {
        this.charsetName = charset.name();
        this.charset = charset;
    }

    /**
     * Custom serialization to handle non-serializable Charset.
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    /**
     * Custom deserialization to recreate the Charset from its name.
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.charset = Charset.forName(charsetName);
    }

    @Override
    public String deserialize(byte[] data, RecordMetadata metadata) throws IOException {
        if (data == null) {
            return null;
        }
        return new String(data, charset);
    }

    @Override
    public TypeDescriptor<String> getProducedType() {
        return TypeDescriptor.of(String.class);
    }

    @Override
    public boolean isNullable() {
        return true;
    }
}
