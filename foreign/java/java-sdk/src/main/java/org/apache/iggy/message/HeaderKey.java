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

package org.apache.iggy.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public record HeaderKey(HeaderKind kind, byte[] value) {
    @JsonCreator
    public static HeaderKey fromJson(@JsonProperty("kind") HeaderKind kind, @JsonProperty("value") String base64Value) {
        byte[] decodedValue = Base64.getDecoder().decode(base64Value);
        return new HeaderKey(kind, decodedValue);
    }

    public static HeaderKey fromString(String val) {
        if (val.isEmpty() || val.length() > 255) {
            throw new IllegalArgumentException("Value has incorrect size, must be between 1 and 255");
        }
        return new HeaderKey(HeaderKind.String, val.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HeaderKey headerKey = (HeaderKey) o;
        return kind == headerKey.kind && Arrays.equals(value, headerKey.value);
    }

    @Override
    public int hashCode() {
        int result = kind.hashCode();
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return new String(value, StandardCharsets.UTF_8);
    }
}
