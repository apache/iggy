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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for ConsumerGroupConfig functionality.
 */
public class ConsumerGroupConfigTest {

    @Test
    public void testDefaultConfig() {
        ConsumerGroupConfig config = ConsumerGroupConfig.builder().build();
        
        assertEquals(Duration.ofSeconds(30), config.getSessionTimeout());
        assertEquals(Duration.ofSeconds(10), config.getHeartbeatInterval());
        assertEquals(RebalanceStrategy.ROUND_ROBIN, config.getRebalanceStrategy());
        assertTrue(config.isAutoCommitEnabled());
        assertEquals(Duration.ofSeconds(5), config.getAutoCommitInterval());
        assertTrue(config.isCreateIfNotExists());
        
        // Test default callbacks don't throw exceptions
        assertDoesNotThrow(() -> config.getOnPartitionsAssigned().accept(List.of(1L, 2L)));
        assertDoesNotThrow(() -> config.getOnPartitionsRevoked().accept(List.of(1L, 2L)));
    }
    
    @Test
    public void testCustomConfig() {
        ConsumerGroupConfig config = ConsumerGroupConfig.builder()
            .sessionTimeout(Duration.ofSeconds(45))
            .heartbeatInterval(Duration.ofSeconds(15))
            .rebalanceStrategy(RebalanceStrategy.ROUND_ROBIN)
            .autoCommit(false)
            .autoCommitInterval(Duration.ofSeconds(10))
            .createIfNotExists(false)
            .build();
        
        assertEquals(Duration.ofSeconds(45), config.getSessionTimeout());
        assertEquals(Duration.ofSeconds(15), config.getHeartbeatInterval());
        assertEquals(RebalanceStrategy.ROUND_ROBIN, config.getRebalanceStrategy());
        assertFalse(config.isAutoCommitEnabled());
        assertEquals(Duration.ofSeconds(10), config.getAutoCommitInterval());
        assertFalse(config.isCreateIfNotExists());
    }
}