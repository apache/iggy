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

package org.apache.iggy.client.async.tcp.vsr;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.util.Map;

import static org.apache.iggy.ResourceOptionsFixture.section;
import static org.assertj.core.api.Assertions.assertThat;

class VsrOperationTest {

    @Test
    void givenFixtureOperationsWhenMirroringShouldMatchCodesAndCorrelation() {
        JsonNode constants = section("constants");

        for (Map.Entry<String, JsonNode> command :
                constants.get("command_codes").properties()) {
            JsonNode operation = command.getValue().get("operation");
            int expected = operation.isNull() ? VsrOperation.NON_REPLICATED : operation.asInt();
            assertThat(VsrOperation.operationForCode(
                            command.getValue().get("code").asInt()))
                    .as(command.getKey())
                    .isEqualTo(expected);
        }
        for (Map.Entry<String, JsonNode> retired :
                constants.get("retired_command_codes").properties()) {
            assertThat(VsrOperation.operationForCode(retired.getValue().asInt()))
                    .as(retired.getKey())
                    .isEqualTo(VsrOperation.NON_REPLICATED);
        }
        for (Map.Entry<String, JsonNode> operation : constants.get("operations").properties()) {
            int value = operation.getValue().get("value").asInt();
            assertThat(VsrOperation.isKnown(value)).as(operation.getKey()).isTrue();
            assertThat(VsrOperation.isMetadata(value)).as(operation.getKey()).isTrue();
            assertThat(VsrOperation.correlationOperation(value))
                    .as(operation.getKey())
                    .isEqualTo(operation.getValue().get("request_operation").asInt());
        }
        for (Map.Entry<String, JsonNode> retired :
                constants.get("retired_operations").properties()) {
            assertThat(VsrOperation.isKnown(retired.getValue().asInt()))
                    .as(retired.getKey())
                    .isFalse();
        }
        assertThat(VsrLoginCodec.PROTOCOL_VERSION)
                .isEqualTo(constants.get("protocol_version").get("packed").asInt())
                .isEqualTo(constants.get("protocol_version_min").get("packed").asInt());
    }
}
