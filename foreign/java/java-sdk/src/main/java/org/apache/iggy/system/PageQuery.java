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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * One page request of a list command. To read the next page, send the same query with the {@link Page#next()} of the
 * page as its cursor. The lists of named entities take a {@link NamedPageQuery}, which adds a name filter.
 *
 * @param order the order of the walk
 * @param descending whether the walk starts at the end of the order
 * @param limit items per page, 1 to 1000
 * @param cursor the next cursor of the previous page, empty for the first page
 */
public record PageQuery(PageOrder order, boolean descending, long limit, Optional<PageCursor> cursor) {

    /** Items per page when the caller does not say. */
    public static final long DEFAULT_LIMIT = 100;

    /** Most items one page returns. */
    public static final long MAX_LIMIT = 1000;

    public PageQuery {
        Objects.requireNonNull(order, "order");
        Objects.requireNonNull(cursor, "cursor");
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The same query after {@code value}, how a walk moves to the next page. */
    public PageQuery withCursor(PageCursor value) {
        return new PageQuery(order, descending, limit, Optional.of(value));
    }

    /**
     * Applies the page rules of the server, so a refused query is never sent: a limit of 1 to {@link #MAX_LIMIT}.
     *
     * @throws org.apache.iggy.exception.IggyValidationException invalid command naming the broken rule
     */
    public void validate() {
        if (limit < 1 || limit > MAX_LIMIT) {
            throw MetadataChangeEncoding.refused("The limit must be 1 to " + MAX_LIMIT + ", got " + limit);
        }
    }

    /**
     * The query keys of an HTTP list route, in a stable order. They always hold the limit, so the server answers one
     * page and not the whole list.
     */
    public Map<String, String> httpQuery() {
        Map<String, String> pairs = new LinkedHashMap<>();
        pairs.put("order", order.value());
        pairs.put("descending", Boolean.toString(descending));
        pairs.put("limit", Long.toString(limit));
        cursor.ifPresent(value -> pairs.put("cursor", value.toString()));
        return pairs;
    }

    /** Builds a query from the defaults: ascending id order, a limit of 100 and no cursor. */
    public static final class Builder {
        private PageOrder order = PageOrder.ID;
        private boolean descending;
        private long limit = DEFAULT_LIMIT;
        private Optional<PageCursor> cursor = Optional.empty();

        private Builder() {}

        public Builder order(PageOrder value) {
            order = value;
            return this;
        }

        public Builder descending(boolean value) {
            descending = value;
            return this;
        }

        public Builder limit(long value) {
            limit = value;
            return this;
        }

        public Builder cursor(PageCursor value) {
            cursor = Optional.of(value);
            return this;
        }

        public PageQuery build() {
            return new PageQuery(order, descending, limit, cursor);
        }
    }
}
