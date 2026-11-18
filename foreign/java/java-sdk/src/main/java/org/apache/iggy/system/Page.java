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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * One page of a list. A page can be short or empty and still have a next cursor, so a walk ends at the first page
 * without one.
 *
 * @param items the items in the order of the query
 * @param next the cursor of the next page, empty on the last page
 * @param <T> the item type
 */
public record Page<T>(List<T> items, Optional<PageCursor> next) {

    public Page {
        items = List.copyOf(items);
        Objects.requireNonNull(next, "next");
    }
}
