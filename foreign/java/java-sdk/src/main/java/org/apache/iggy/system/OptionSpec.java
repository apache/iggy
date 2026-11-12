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

package org.apache.iggy.system;

import org.apache.iggy.message.HeaderValue;

/**
 * One entry of a resource's option catalog.
 *
 * <p>This is the discovery surface for the option keys a create or update command accepts: a key outside
 * the catalog is refused, and the binary transports carry only the error code back, so there is no other
 * way to learn which keys a given server knows.
 *
 * @param key the option key
 * @param defaultValue the key's default, carrying the kind the server encodes it under, or empty for
 *     a key with no default. Reuses {@link HeaderValue} because options ride the user-headers codec.
 * @param description what the option does, including the bounds its value is checked against
 * @param flags what an update may do with the key, as {@code FLAG_*} bits
 */
public record OptionSpec(String key, HeaderValue defaultValue, String description, int flags) {

    /** An update may set the key. */
    public static final int FLAG_UPDATABLE = 1;

    /** An update may reset the key to its default. */
    public static final int FLAG_RESETTABLE = 1 << 1;

    /** Write-only: accepted, never returned. */
    public static final int FLAG_SECRET = 1 << 2;

    /** Computed by the server and returned on reads, never accepted. */
    public static final int FLAG_READ_ONLY = 1 << 3;

    /** A node that does not understand the key refuses to serve the resource. */
    public static final int FLAG_CRITICAL = 1 << 4;

    public boolean updatable() {
        return (flags & FLAG_UPDATABLE) != 0;
    }

    public boolean resettable() {
        return (flags & FLAG_RESETTABLE) != 0;
    }

    public boolean secret() {
        return (flags & FLAG_SECRET) != 0;
    }

    public boolean readOnly() {
        return (flags & FLAG_READ_ONLY) != 0;
    }

    public boolean critical() {
        return (flags & FLAG_CRITICAL) != 0;
    }
}
