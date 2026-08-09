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

package org.apache.iggy.cluster;

/**
 * A single node in the cluster roster.
 *
 * @param name      the node name
 * @param ip        the node IP address or hostname
 * @param endpoints the per-transport ports of the node
 * @param role      the node role
 * @param status    the node status
 */
public record ClusterNode(
        String name, String ip, TransportEndpoints endpoints, ClusterNodeRole role, ClusterNodeStatus status) {}
