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

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.iggy.exception.IggyOperationNotSupportedException;
import org.apache.iggy.message.HeaderEntry;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import tools.jackson.databind.annotation.JsonSerialize;

import java.util.List;
import java.util.Map;

/**
 * Jackson mixin for {@link Message} to keep the domain object free of serialization annotations.
 */
abstract class MessageMixin {

    @JsonCreator
    static Message of(MessageHeader header, byte[] payload, List<HeaderEntry> userHeaders) {
        throw new IggyOperationNotSupportedException("Mixin method should not be called directly");
    }

    @JsonSerialize(using = UserHeadersSerializer.class)
    abstract Map<HeaderKey, HeaderValue> userHeaders();
}
