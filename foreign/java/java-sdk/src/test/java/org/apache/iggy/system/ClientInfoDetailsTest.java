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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ClientInfoDetailsTest {
    @Test
    void constructorWithClientInfoCreatesExpectedClientInfoDetails() {
        var clientInfo = new ClientInfo(321L, Optional.of(100L), "address", "transport", 1L);
        var consumerGroupInfo = List.of(new ConsumerGroupInfo(1L, 2L, 3L));

        var clientInfoDetails = new ClientInfoDetails(clientInfo, consumerGroupInfo);

        assertThat(clientInfoDetails.clientId()).isEqualTo(321L);
        assertThat(clientInfoDetails.userId()).isEqualTo(Optional.of(100L));
        assertThat(clientInfoDetails.address()).isEqualTo("address");
        assertThat(clientInfoDetails.transport()).isEqualTo("transport");
        assertThat(clientInfoDetails.consumerGroupsCount()).isEqualTo(1L);
        assertThat(clientInfoDetails.consumerGroups()).isEqualTo(consumerGroupInfo);
    }
}
