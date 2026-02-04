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

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import org.apache.iggy.message.Message;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.EnumNamingStrategies;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;

final class ObjectMapperFactory {

    private static final ObjectMapper INSTANCE = JsonMapper.builder()
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .enable(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES)
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .enumNamingStrategy(EnumNamingStrategies.LOWER_CASE)
            .withConfigOverride(Map.class, map -> map.setNullHandling(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY)))
            .withConfigOverride(
                    List.class, list -> list.setNullHandling(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY)))
            .addMixIn(Message.class, MessageMixin.class)
            .build();

    private ObjectMapperFactory() {}

    static ObjectMapper getInstance() {
        return INSTANCE;
    }
}
