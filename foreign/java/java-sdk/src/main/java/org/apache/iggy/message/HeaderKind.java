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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iggy.exception.IggyInvalidArgumentException;

public enum HeaderKind {
    @JsonProperty("raw")
    Raw(1),
    @JsonProperty("string")
    String(2),
    @JsonProperty("bool")
    Bool(3),
    @JsonProperty("int8")
    Int8(4),
    @JsonProperty("int16")
    Int16(5),
    @JsonProperty("int32")
    Int32(6),
    @JsonProperty("int64")
    Int64(7),
    @JsonProperty("int128")
    Int128(8),
    @JsonProperty("uint8")
    Uint8(9),
    @JsonProperty("uint16")
    Uint16(10),
    @JsonProperty("uint32")
    Uint32(11),
    @JsonProperty("uint64")
    Uint64(12),
    @JsonProperty("uint128")
    Uint128(13),
    @JsonProperty("float32")
    Float32(14),
    @JsonProperty("float64")
    Float64(15);

    private final int code;

    HeaderKind(int code) {
        this.code = code;
    }

    public static HeaderKind fromCode(int code) {
        for (HeaderKind kind : values()) {
            if (kind.code == code) {
                return kind;
            }
        }
        throw new IggyInvalidArgumentException("Unknown header kind: " + code);
    }

    public int asCode() {
        return code;
    }
}
