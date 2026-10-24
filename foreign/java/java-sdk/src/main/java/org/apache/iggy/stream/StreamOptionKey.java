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

package org.apache.iggy.stream;

import org.apache.iggy.system.OptionKey;

import static org.apache.iggy.system.OptionSpec.FLAG_CRITICAL;
import static org.apache.iggy.system.OptionSpec.FLAG_READ_ONLY;
import static org.apache.iggy.system.OptionSpec.FLAG_SECRET;
import static org.apache.iggy.system.OptionSpec.FLAG_UPDATABLE;

/**
 * The option keys of a stream, in catalog order.
 *
 * <p>{@code name} is set at create and changed through an update. {@code encryption_key} is accepted only
 * at create and never returned. The three projections are computed by the server and only read.
 */
public enum StreamOptionKey implements OptionKey {
    NAME("name", FLAG_UPDATABLE),
    ENCRYPTION_KEY("encryption_key", FLAG_SECRET | FLAG_CRITICAL),
    ENCRYPTION_KEY_SOURCE("encryption_key_source", FLAG_READ_ONLY),
    ENCRYPTION_KEY_FINGERPRINT("encryption_key_fingerprint", FLAG_READ_ONLY),
    ENCRYPTION_KEK_FINGERPRINT("encryption_kek_fingerprint", FLAG_READ_ONLY);

    private final String value;
    private final int flags;

    StreamOptionKey(String value, int flags) {
        this.value = value;
        this.flags = flags;
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public int flags() {
        return flags;
    }
}
