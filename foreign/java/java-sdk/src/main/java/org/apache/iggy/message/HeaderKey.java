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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.iggy.exception.IggyInvalidArgumentException;

import java.nio.charset.StandardCharsets;

public record HeaderKey(HeaderKind kind, byte[] value) {

    public static HeaderKey fromString(String value) {
        if (StringUtils.isBlank(value)) {
            throw new IggyInvalidArgumentException("Value cannot be null or empty");
        }
        var bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 255) {
            throw new IggyInvalidArgumentException("Value has incorrect size, must be between 1 and 255 bytes");
        }
        return new HeaderKey(HeaderKind.String, bytes);
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
        return new EqualsBuilder()
                .append(value, headerKey.value)
                .append(kind, headerKey.kind)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(kind).append(value).toHashCode();
    }

    @Override
    public String toString() {
        return new String(value, StandardCharsets.UTF_8);
    }
}
