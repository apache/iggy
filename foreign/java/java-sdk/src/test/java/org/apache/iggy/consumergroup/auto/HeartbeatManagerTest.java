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

import org.apache.iggy.client.blocking.SystemClient;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;

import static org.mockito.Mockito.*;

/**
 * Test for HeartbeatManager functionality.
 */
public class HeartbeatManagerTest {

    @Test
    public void testHeartbeatManagerStartsAndStops() {
        SystemClient systemClient = Mockito.mock(SystemClient.class);
        Duration interval = Duration.ofMillis(100);
        
        try (HeartbeatManager manager = new HeartbeatManager(systemClient, interval)) {
            // Start the manager
            manager.start();
            
            // Wait a bit to allow heartbeat to be sent
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Verify that ping was called
            verify(systemClient, atLeastOnce()).ping();
            
            // Stop the manager
            manager.stop();
        }
    }
    
    @Test
    public void testHeartbeatManagerClose() {
        SystemClient systemClient = Mockito.mock(SystemClient.class);
        Duration interval = Duration.ofMillis(100);
        
        HeartbeatManager manager = new HeartbeatManager(systemClient, interval);
        manager.start();
        
        // Close the manager
        manager.close();
        
        // Verify that ping was called at least once
        verify(systemClient, atLeastOnce()).ping();
    }
}