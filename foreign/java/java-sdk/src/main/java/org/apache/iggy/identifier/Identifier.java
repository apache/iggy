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

package org.apache.iggy.identifier;

import org.apache.commons.lang3.StringUtils;
import org.apache.iggy.exception.IggyInvalidArgumentException;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public abstract class Identifier {

    /** Server-side cap on a wire name, in UTF-8 bytes, matching its u8 length prefix. */
    public static final int MAX_NAME_LENGTH = 255;

    private final String name;
    private final byte[] encodedName;
    private final Long id;

    protected Identifier(@Nullable String name, @Nullable Long id) {
        if (StringUtils.isBlank(name) && id == null) {
            throw new IggyInvalidArgumentException("Name and id cannot be blank");
        }
        if (StringUtils.isNotBlank(name) && id != null) {
            throw new IggyInvalidArgumentException("Name and id cannot be both present");
        }
        if (StringUtils.isNotBlank(name)) {
            byte[] encoded = name.getBytes(StandardCharsets.UTF_8);
            if (encoded.length > MAX_NAME_LENGTH) {
                throw new IggyInvalidArgumentException(
                        "Name must be at most " + MAX_NAME_LENGTH + " bytes, got " + encoded.length);
            }
            this.name = name;
            this.encodedName = encoded;
            this.id = null;
        } else {
            this.name = null;
            this.encodedName = null;
            this.id = id;
        }
    }

    @Override
    public String toString() {
        if (StringUtils.isNotBlank(name)) {
            return name;
        }
        return id.toString();
    }

    public int getKind() {
        if (id != null) {
            return 1;
        }
        return 2;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    /**
     * The name as UTF-8 wire bytes, encoded once at construction; {@code null} for a numeric
     * identifier. The array is shared rather than copied, so callers must not modify it.
     */
    @Nullable public byte[] getEncodedName() {
        return encodedName;
    }

    public int getSize() {
        if (id != null) {
            // kind, 1 byte + length, 1 byte + id, 4 bytes
            return 6;
        } else {
            // kind, 1 byte + length, 1 byte + encoded name bytes
            return 2 + encodedName.length;
        }
    }
}
