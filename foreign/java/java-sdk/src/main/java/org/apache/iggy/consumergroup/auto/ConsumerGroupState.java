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

/**
 * Represents the state of a consumer group.
 */
public enum ConsumerGroupState {
    /**
     * Consumer group has been created but not initialized.
     */
    CREATED,
    
    /**
     * Consumer group is being initialized.
     */
    INITIALIZING,
    
    /**
     * Consumer group is joining the group.
     */
    JOINING,
    
    /**
     * Consumer group is active and ready to consume messages.
     */
    ACTIVE,
    
    /**
     * Consumer group is polling for messages.
     */
    POLLING,
    
    /**
     * Consumer group is rebalancing partitions.
     */
    REBALANCING,
    
    /**
     * Consumer group has lost connection to the server.
     */
    DISCONNECTED,
    
    /**
     * Consumer group is reconnecting to the server.
     */
    RECONNECTING,
    
    /**
     * Consumer group is leaving the group.
     */
    LEAVING
}