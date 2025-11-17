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

import javax.annotation.Nullable;

public abstract class Identifier {

    private final String name;
    private final Long id;

    protected Identifier(@Nullable String name, @Nullable Long id) {
        if (StringUtils.isBlank(name) && id == null) {
            throw new IllegalArgumentException("Name and id cannot be blank");
        }
        if (StringUtils.isNotBlank(name) && id != null) {
            throw new IllegalArgumentException("Name and id cannot be both present");
        }
        if (StringUtils.isNotBlank(name)) {
            this.name = name;
            this.id = null;
        } else {
            this.name = null;
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

    public int getSize() {
        if (id != null) {
            // kind, 1 byte + length, 1 byte + id, 4 bytes
            return 6;
        } else {
            // kind, 1 byte + length, 1 byte + name.length()
            return 2 + name.length();
        }
    }
}
