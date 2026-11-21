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

import org.apache.iggy.exception.IggyInvalidArgumentException;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/** Encodes a name as one path segment of a request URL. */
final class PathSegment {

    private PathSegment() {}

    /**
     * {@code value} escaped so that it stays one segment.
     *
     * @param value the name to encode
     * @param what what the name is, for the refusal message
     * @throws IggyInvalidArgumentException for a name no segment can carry unchanged
     */
    static String encode(String value, String what) {
        if (value == null) {
            throw new IggyInvalidArgumentException(what + " must not be null");
        }
        // The URL parser resolves "." and ".." even when percent-encoded, so the request would hit another route.
        if (".".equals(value) || "..".equals(value)) {
            throw new IggyInvalidArgumentException(what + " must not be '.' or '..'");
        }
        // The encoder turns a lone surrogate into '?', which would name another resource.
        if (!value.equals(new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8))) {
            throw new IggyInvalidArgumentException(what + " must be valid Unicode");
        }
        // URLEncoder writes a space as '+', which in a path means a literal plus.
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
