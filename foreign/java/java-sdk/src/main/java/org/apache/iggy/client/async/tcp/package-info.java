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

/**
 * Netty-based TCP implementation of the async Iggy client.
 *
 * <p>This package provides the concrete implementation of the async client interfaces
 * using Netty for non-blocking TCP communication with the Iggy server binary protocol.
 *
 * <h2>Key Classes</h2>
 * <ul>
 *   <li>{@link org.apache.iggy.client.async.tcp.AsyncIggyTcpClient} — main client entry
 *       point; provides access to all sub-clients</li>
 *   <li>{@link org.apache.iggy.client.async.tcp.AsyncIggyTcpClientBuilder} — fluent builder
 *       for configuring and constructing the client</li>
 *   <li>{@link org.apache.iggy.client.async.tcp.AsyncTcpConnection} — manages the Netty
 *       channel, request serialization, and response correlation</li>
 * </ul>
 *
 * <h2>Protocol Details</h2>
 * <p>The transport speaks the VSR (Viewstamped Replication) wire protocol:
 * every frame starts with a 256-byte consensus header ({@code RequestHeader}
 * on the way out, {@code ReplyHeader} or {@code EvictionHeader} on the way
 * back) followed by the command payload; see the {@code vsr} subpackage.
 * <p>Responses are matched to requests in FIFO order. The
 * {@link org.apache.iggy.client.async.tcp.AsyncTcpConnection}
 * serializes all writes through Netty's event loop to maintain ordering.
 *
 * @see org.apache.iggy.client.async.tcp.AsyncIggyTcpClient
 */
package org.apache.iggy.client.async.tcp;
