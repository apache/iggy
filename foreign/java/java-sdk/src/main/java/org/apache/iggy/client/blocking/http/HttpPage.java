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

package org.apache.iggy.client.blocking.http;

import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.system.NamedPageQuery;
import org.apache.iggy.system.Page;
import org.apache.iggy.system.PageCursor;
import tools.jackson.core.type.TypeReference;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/** The JSON page of a list route, {@code {"items": [...], "next": "pg1_..."}}, with no next on the last page. */
record HttpPage<T>(List<T> items, Optional<String> next) {

    /** Reads one page of {@code path}. A query that {@link NamedPageQuery#validate()} refuses is never sent. */
    static <T> HttpPage<T> read(
            InternalHttpClient httpClient, String path, NamedPageQuery query, TypeReference<HttpPage<T>> type) {
        query.validate();
        NameValuePair[] params = query.httpQuery().entrySet().stream()
                .map(pair -> new BasicNameValuePair(pair.getKey(), pair.getValue()))
                .toArray(NameValuePair[]::new);
        return httpClient.execute(httpClient.prepareGetRequest(path, params), type);
    }

    <R> Page<R> toPage(Function<T, R> toItem) {
        Optional<PageCursor> cursor;
        try {
            cursor = next.map(PageCursor::parse);
        } catch (IggyValidationException refused) {
            throw new IggyMalformedResponseException("Malformed page: next", refused);
        }
        return new Page<>(items.stream().map(toItem).toList(), cursor);
    }
}
