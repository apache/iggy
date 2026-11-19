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

import org.apache.iggy.message.HeaderValue;

import java.util.Map;
import java.util.Optional;

/** Reads the origin a server reports in the derived options of a shadow stream. */
public final class ShadowProjection {

    private ShadowProjection() {}

    /** The geo origin whose topics the stream serves, empty for a stream that is no shadow. */
    public static Optional<String> origin(Map<String, HeaderValue> options, Map<String, HeaderValue> derived) {
        return EncryptionProjection.text(options, derived, StreamOptionKey.SHADOW_ORIGIN);
    }
}
