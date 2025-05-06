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

package rs.iggy.message;

public enum HeaderKind {
    Raw(1),
    String(2),
    Bool(3),
    Int8(4),
    Int16(5),
    Int32(6),
    Int64(7),
    Int128(8),
    Uint8(9),
    Uint16(10),
    Uint32(11),
    Uint64(12),
    Uint128(13),
    Float32(14),
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
        throw new IllegalArgumentException("Unknown header kind: " + code);
    }

    public int asCode() {
        return code;
    }
}
