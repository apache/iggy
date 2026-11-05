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

package org.apache.iggy.user;

import org.apache.iggy.system.OptionKey;

import static org.apache.iggy.system.OptionSpec.FLAG_UPDATABLE;

/**
 * The option keys of a user, in catalog order. Both are changed through an update and neither resets.
 */
public enum UserOptionKey implements OptionKey {
    USERNAME("username", FLAG_UPDATABLE),
    STATUS("status", FLAG_UPDATABLE);

    private final String value;
    private final int flags;

    UserOptionKey(String value, int flags) {
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
