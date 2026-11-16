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

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * One page request of a list of named entities: streams, topics, consumer groups and users.
 *
 * @param page the order, limit and cursor of the page
 * @param name keeps only the entries whose name holds this text, case-sensitive, empty for every entry
 */
public record NamedPageQuery(PageQuery page, Optional<String> name) {

    /** Most bytes of a name filter in UTF-8. */
    public static final int MAX_NAME_SIZE = 255;

    public NamedPageQuery {
        Objects.requireNonNull(page, "page");
        Objects.requireNonNull(name, "name");
    }

    /** The query of {@code page} without a name filter. */
    public NamedPageQuery(PageQuery page) {
        this(page, Optional.empty());
    }

    /** The same query after {@code value}, how a walk moves to the next page. */
    public NamedPageQuery withCursor(PageCursor value) {
        return new NamedPageQuery(page.withCursor(value), name);
    }

    /**
     * Applies {@link PageQuery#validate()} and the name rule of the server, so a refused query is never sent: a name of
     * 1 to {@link #MAX_NAME_SIZE} bytes of UTF-8.
     *
     * @throws org.apache.iggy.exception.IggyValidationException invalid command naming the broken rule
     */
    public void validate() {
        page.validate();
        if (name.isEmpty()) {
            return;
        }
        // A lone surrogate has no UTF-8 form, so the server would read another name.
        if (!StandardCharsets.UTF_8.newEncoder().canEncode(name.get())) {
            throw MetadataChangeEncoding.refused("The name must be valid Unicode text");
        }
        int size = name.get().getBytes(StandardCharsets.UTF_8).length;
        if (size < 1 || size > MAX_NAME_SIZE) {
            throw MetadataChangeEncoding.refused(
                    "The name must be 1 to " + MAX_NAME_SIZE + " bytes of UTF-8, got " + size);
        }
    }

    /** The {@link PageQuery#httpQuery()} keys of the page, and the name filter last. */
    public Map<String, String> httpQuery() {
        Map<String, String> pairs = new LinkedHashMap<>(page.httpQuery());
        name.ifPresent(value -> pairs.put("name", value));
        return pairs;
    }
}
