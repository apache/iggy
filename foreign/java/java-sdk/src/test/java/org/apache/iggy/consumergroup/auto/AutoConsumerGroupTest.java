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

package org.apache.iggy.consumergroup.auto;

import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test for AutoConsumerGroup functionality.
 */
public class AutoConsumerGroupTest {

    @Test
    public void testBuilderCreatesValidInstance() {
        // Skip this test in normal runs as it requires a running Iggy server
        assumeTrue(false, "Skipping test - requires running Iggy server");
        
        IggyBaseClient client = new IggyTcpClient("localhost", 8090);
        
        AutoConsumerGroup consumerGroup = client.consumerGroup()
            .stream(1L)
            .topic(1L)
            .groupName(1L)
            .build();
        
        assertNotNull(consumerGroup);
        assertEquals(ConsumerGroupState.CREATED, consumerGroup.getState());
    }
    
    @Test
    public void testConfigBuilder() {
        ConsumerGroupConfig config = ConsumerGroupConfig.builder()
            .sessionTimeout(Duration.ofSeconds(45))
            .heartbeatInterval(Duration.ofSeconds(15))
            .rebalanceStrategy(RebalanceStrategy.ROUND_ROBIN)
            .autoCommit(true)
            .autoCommitInterval(Duration.ofSeconds(10))
            .createIfNotExists(true)
            .build();
        
        assertEquals(Duration.ofSeconds(45), config.getSessionTimeout());
        assertEquals(Duration.ofSeconds(15), config.getHeartbeatInterval());
        assertEquals(RebalanceStrategy.ROUND_ROBIN, config.getRebalanceStrategy());
        assertEquals(true, config.isAutoCommitEnabled());
        assertEquals(Duration.ofSeconds(10), config.getAutoCommitInterval());
        assertEquals(true, config.isCreateIfNotExists());
    }
}